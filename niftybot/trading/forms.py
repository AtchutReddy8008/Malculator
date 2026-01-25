from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User
from .models import Broker

class SignUpForm(UserCreationForm):
    email = forms.EmailField(required=True, max_length=254)
    full_name = forms.CharField(max_length=100, required=False)
    mobile_number = forms.CharField(max_length=15, required=False)
    
    class Meta:
        model = User
        fields = ('username', 'email', 'password1', 'password2', 'full_name', 'mobile_number')
    
    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        if commit:
            user.save()
        return user

class ZerodhaConnectionForm(forms.ModelForm):
    class Meta:
        model = Broker
        fields = ('api_key', 'secret_key', 'zerodha_user_id', 'password', 'totp')
        widgets = {
            'api_key': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter API Key'}),
            'secret_key': forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Enter Secret Key'}),
            'zerodha_user_id': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter Zerodha User ID'}),
            'password': forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Enter Password'}),
            'totp': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter TOTP Secret'}),
        }
    
    def clean(self):
        cleaned_data = super().clean()
        # Add any validation if needed
        return cleaned_data